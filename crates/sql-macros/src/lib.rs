use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(FromRow, attributes(db))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let Data::Struct(data) = input.data else {
        return syn::Error::new_spanned(name, "FromRow requires a struct")
            .into_compile_error()
            .into();
    };
    let Fields::Named(fields) = data.fields else {
        return syn::Error::new_spanned(name, "FromRow requires named fields")
            .into_compile_error()
            .into();
    };

    let crate_path = match crate_name("ofdb-sql") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name.replace('-', "_"));
            quote!(::#ident)
        }
        Err(_) => quote!(::ofdb_sql),
    };

    let fields = fields.named.iter().map(|field| {
        let ident = field.ident.as_ref().expect("named fields have identifiers");
        let ty = &field.ty;
        let mut column = ident.to_string();
        for attribute in &field.attrs {
            if !attribute.path().is_ident("db") {
                continue;
            }
            let result = attribute.parse_nested_meta(|meta| {
                if !meta.path.is_ident("column") {
                    return Err(meta.error("expected column"));
                }
                column = meta.value()?.parse::<syn::LitStr>()?.value();
                Ok(())
            });
            if let Err(error) = result {
                return error.into_compile_error();
            }
        }
        quote! {
            #ident: #crate_path::decode::<#ty>(#crate_path::value(row, columns, #column)?, #column)?
        }
    });

    quote! {
        impl #crate_path::FromRow for #name {
            fn from_row(
                row: &#crate_path::Row,
                columns: &[&str],
            ) -> ::core::result::Result<Self, #crate_path::FromRowError> {
                ::core::result::Result::Ok(Self { #(#fields),* })
            }
        }
    }
    .into()
}
